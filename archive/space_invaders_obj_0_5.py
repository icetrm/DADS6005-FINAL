import pygame
import random
import math

# Define Classes

class SpaceObject:

	def __init__(self, image, posX=0, posY=0, speedX = 0, speedY = 0, sizeX = 64, sizeY = 64, state = 'show', sound = ' '):
		#self.namme	= name
		self.image  = image
		self.sizeX  = sizeX
		self.sizeY  = sizeY
		self.posX   = posX
		self.posY   = posY
		self.speedX = speedX
		self.speedY	= speedY
		self.state	= state		# 'hide', 'show'
		self.sound 	= sound

	def show(self):
		if self.state == 'show':
			screen.blit(self.image, (int(self.posX), int(self.posY)))



class SpaceShip(SpaceObject):
    
    # def __init__(self):
    #     super().__init__()

	def update_player_postion(self, screen_sizeX, screen_sizeY):

		# Update X position (update with min/max)
		self.posX += self.speedX
		if self.posX < 0:
			self.posX = 0
		elif self.posX > screen_sizeX-self.sizeX:
			self.posX = screen_sizeX-self.sizeX

		# Update Y position (update with min/max)
		self.posY += self.speedY
		if self.posY < 0:
			self.posY = 0
		elif self.posY > screen_sizeY-self.sizeY:
			self.posY = screen_sizeY-self.sizeY


class SpaceEnemy(SpaceObject):

	def update_enemy_position(self, screen_sizeX, screen_sizeY):

		# Update X position
		self.posX += self.speedX

		# Update Y position
		self.posY += self.speedY

class Bullet(SpaceObject):

	def update_bullet_position(self, screen_sizeX, screen_sizeY):

		# Update X position
		self.posX += self.speedX

		# Update Y position, and change state if outside screen
		self.posY += self.speedY
		if self.posY < -self.sizeY:
			self.state = 'hide'


	def fire_bullet(self, player):

		self.posX = player.posX + player.sizeX/2 - self.sizeX/2
		self.posY = player.posY
		self.sound.play()
		self.state = 'show'


# Define Procedures

def enemy_respawn(enemy, level):
#	enemy.image = enemy_images[0] 
	enemy.posX 	= random.randint(0, screen_sizeX) 
	enemy.posY 	= random.randint(-screen_sizeY, -100)
	enemy.speedX = random.randint(-10, 10) / 10
	enemy.speedY = level
	
def is_collision(object1, object2):

	obj1_midX = object1.posX + object1.sizeX
	obj1_midY = object1.posY + object1.sizeY
	obj2_midX = object2.posX + object2.sizeX
	obj2_midY = object2.posY + object2.sizeY

	# think if I want to improve this...
	distance = math.sqrt(math.pow(obj1_midX-obj2_midX,2) + math.pow(obj1_midY-obj2_midY,2))
	collision_limit = (object1.sizeX + object1.sizeY + object2.sizeX + object2.sizeY) / 5

	return distance < collision_limit

def show_score(score, level, score_font, font_size = 24, x=10, y=10):
	font_size = 24
	level_text = score_font.render("Level : " + str(level), True, (0, 255, 0))
	score_text = score_font.render("Score : " + str(score), True, (0, 255, 0))
	screen.blit(level_text, (x, y))
	screen.blit(score_text, (x, y + 5 + font_size))


def show_game_over(screen_sizeX, screen_sizeY):
	
	# Move enemies below screen (is there a better way?)
	for i in range(num_of_enemies):
		enemies[i].posY = screen_sizeY + 100

	# Game over font
	game_over_font = pygame.font.Font('freesansbold.ttf', 64)
	game_over_text = game_over_font.render('GAME OVER', True, (255, 0, 0))
	screen.blit(game_over_text, (int(screen_sizeX/2-200), int(screen_sizeY/2-75)))


#########################
####   Main Program   ###
#########################
#if __name__ == '__main__':

# initialize pygame 
pygame.init()

# Initialize Global variables
screen_sizeX = 800
screen_sizeY = 600
screen_size = (screen_sizeX, screen_sizeY)
background_color = (0, 0, 0)
# Initialize screen
screen = pygame.display.set_mode((screen_sizeX, screen_sizeY))

# Initialize fonts
score_font = pygame.font.Font('freesansbold.ttf', 32)

# Initialize images
#example_image     = pygame.image.load('.\\images\\') 
icon_image			= pygame.image.load('.\\images\\space-invader-icon.png') 
background_image	= pygame.image.load('.\\images\\Google-Map-solar-system.jpg')
player_image		= pygame.image.load('.\\images\\spaceship.png')
bullet_image		= pygame.image.load('.\\images\\bullet.png')
enemy_images	    = [pygame.image.load('.\\images\\enemy.png'),
				       pygame.image.load('.\\images\\ufo.png'),
					   pygame.image.load('.\\images\\yoda.png')]

# Initialize sounds
bullet_sound		= pygame.mixer.Sound('.\\sounds\\laser.wav')
explosion_sound		= pygame.mixer.Sound('.\\sounds\\explosion.wav')
pygame.mixer.music.load('.\\sounds\\background.wav')
pygame.mixer.music.play(-1)


# Initialize settings
player_maxSpeedX = 3
player_maxSpeedY = 3
enemy_maxSpeedX = 2
enemy_maxSpeedY = 2

# Game settings
level			= 1
num_of_enemies	= 10
score			= 0
level_change	= 1000
enemy_level_increase = 5

# initialize other variables
level_iter	 	= 0
keyX_pressed = 0
keyY_pressed = 0
game_over 	 = False

# initialize player and bullet
player = SpaceShip(player_image, screen_sizeX/2-32, screen_sizeY-100)
bullet = Bullet(bullet_image, speedY = -7, sound = bullet_sound, state = 'hide', sizeX = 32, sizeY = 32)

# initialize enemies
enemies = []
for i in range(num_of_enemies):
	enemy_image_index = level % len(enemy_images)	
#	enemy_image_index = random.randint(1,len(enemy_images)) - 1
	enemies.append(SpaceEnemy(enemy_images[enemy_image_index]))
	enemy_respawn(enemies[i], level)

# --------------
# Main Game Loop
# --------------

running = True

while running == True:
	
	# check if increase level
	level_iter += 1
	if level_iter > level_change and not game_over:
		level_iter = 0
		level += 1
		for i in range(num_of_enemies, num_of_enemies+enemy_level_increase):
			enemy_image_index = level % len(enemy_images)
			print('enemy_image_index = ', enemy_image_index)
			#	enemy_image_index = random.randint(1,len(enemy_images) - 1)
			enemies.append(SpaceEnemy(enemy_images[enemy_image_index]))
			print('len = ', len(enemies))
			enemy_respawn(enemies[i], level)
		num_of_enemies	+= enemy_level_increase
		print('nu_of_enemies = ', num_of_enemies)

	# Fill screen and background image	
	screen.fill(background_color)
	screen.blit(background_image, (0,0))

	# Check events and take action
	for event in pygame.event.get():	
		if event.type == pygame.QUIT:
			running = False	

		# if key is pressed
		if event.type == pygame.KEYDOWN:
			# if arrow key is pressed
			if event.key == pygame.K_LEFT:
				player.speedX = -player_maxSpeedX
				keyX_pressed += 1
			if event.key == pygame.K_RIGHT:
				player.speedX = player_maxSpeedX
				keyX_pressed += 1
			if event.key == pygame.K_UP:
				player.speedY = -player_maxSpeedY
				keyY_pressed += 1
			if event.key == pygame.K_DOWN:
				player.speedY = player_maxSpeedY
				keyY_pressed += 1

			# if space key, fire bullet
			if event.key == pygame.K_SPACE and bullet.state == 'hide':
				bullet.fire_bullet(player)

		if event.type == pygame.KEYUP:
			if event.key == pygame.K_LEFT or event.key == pygame.K_RIGHT:
				keyX_pressed -= 1
				if keyX_pressed == 0:
					player.speedX = 0
			if event.key == pygame.K_UP or event.key == pygame.K_DOWN:
				keyY_pressed -= 1
				if keyY_pressed == 0:
					player.speedY = 0

	# Move player and check not out of screen
	player.update_player_postion(screen_sizeX, screen_sizeY)
	bullet.update_bullet_position(screen_sizeX, screen_sizeY)

	if game_over:
		# add explosion picutre here
		show_game_over(screen_sizeX, screen_sizeY)
		show_score(score, level, score_font)
	else:
		# Move enemies and check collisions
		for i in range(num_of_enemies):
			enemies[i].update_enemy_position(screen_sizeX, screen_sizeY)
			if enemies[i].posY > screen_sizeY:
				enemy_respawn(enemies[i], level)
			enemies[i].show()
			if is_collision(enemies[i], player):
				explosion_sound.play()
				game_over = True
				break
			elif bullet.state == 'show' and is_collision(enemies[i], bullet) :
				explosion_sound.play()
				score += 1
				bullet.state = 'hide'
				enemy_respawn(enemies[i], level)				
			enemies[i].show()

		# show player
		bullet.show()
		player.show()
		show_score(score, level, score_font)
	
	pygame.display.update()


print('Successfully quit!')